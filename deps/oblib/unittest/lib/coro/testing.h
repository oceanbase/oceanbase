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

#include "lib/coro/co.h"
#include "lib/coro/co_set_sched.h"
#include "lib/thread/thread_pool.h"
#include <functional>
#include <vector>

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace cotesting {
using VoidFuncT = std::function<void()>;
using Idx1FuncT = std::function<void(int)>;       // (grp_th_idx)
using Idx2FuncT = std::function<void(int, int)>;  // (grp_th_idx, grp_co_idx)

//// Worker Definition
//
struct WorkerIndex {
  WorkerIndex()
      : th_idx_(0),      // thread index of all thread
        co_idx_(0),      // routine index of all co-routine
        grp_idx_(0),     // group index
        grp_th_idx_(0),  // thread index of current group
        grp_co_idx_(0),  // routine index of current group
        th_co_idx_(0)    // routine index of current thread
  {}
  int th_idx_;
  int co_idx_;
  int grp_idx_;
  int grp_th_idx_;
  int grp_co_idx_;
  int th_co_idx_;
};

class SimpleWorker : public CoSetSched::Worker {
public:
  SimpleWorker(CoSetSched& sched, VoidFuncT func) : CoSetSched::Worker(sched, func)
  {}
  SimpleWorker(CoSetSched& sched, Idx1FuncT func) : CoSetSched::Worker(sched, [this, func] { func(wi_.grp_co_idx_); })
  {}
  SimpleWorker(CoSetSched& sched, Idx2FuncT func)
      : CoSetSched::Worker(sched, [this, func] { func(wi_.grp_th_idx_, wi_.grp_co_idx_); })
  {}

public:
  WorkerIndex wi_;
};

//// Single Thread Scheduler definition
//
class SimpleSched : public CoSetSched {
  using WorkerT = SimpleWorker;

public:
  template <class FuncT>
  SimpleSched(FuncT func, int cnt, WorkerIndex& wi) : cnt_(cnt), workers_(), wi_(wi)
  {
    for (int i = 0; i < cnt_; i++) {
      auto w = new WorkerT(*this, func);
      workers_.push_back(w);
      w->wi_ = wi_;
      wi_.co_idx_++;
      wi_.grp_co_idx_++;
      wi_.th_co_idx_++;
    }
    wi_.th_co_idx_ = 0;
  }
  ~SimpleSched()
  {
    destroy();
    for (auto w : workers_) {
      delete w;
    }
  }
  int start(bool wait = true)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(CoSetSched::start())) {
    } else if (wait) {
      CoSetSched::wait();
    }
    return ret;
  }

private:
  int prepare() final
  {
    int ret = CoSetSched::create_set(0);
    for (int i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      ret = workers_[i]->init();
    }
    return ret;
  }

private:
  int cnt_;
  std::vector<WorkerT*> workers_;
  WorkerIndex& wi_;
};

//// Flexible worker pool
//
class FlexPool {
  using Sched = SimpleSched;

public:
  FlexPool()
  {}

  template <class FuncT>
  FlexPool(FuncT func, int thes, int coros = 1)
  {
    create(func, thes, coros);
  }

  ~FlexPool()
  {
    for (auto th : th_pools_) {
      delete th;
    }
  }

  template <class FuncT>
  FlexPool& create(FuncT func, int thes, int coros = 1)
  {
    for (int i = 0; i < thes; i++) {
      th_pools_.push_back(new Sched(func, coros, wi_));
      wi_.grp_th_idx_++;
      wi_.th_idx_++;
    }
    wi_.grp_idx_++;
    wi_.grp_th_idx_ = 0;
    wi_.grp_co_idx_ = 0;
    return *this;
  }

  void start(bool wait = true)
  {
    for (auto th : th_pools_) {
      th->start(false);
    }
    if (wait) {
      this->wait();
    }
  }

  void wait()
  {
    for (auto th : th_pools_) {
      th->wait();
    }
  }

private:
  std::vector<Sched*> th_pools_;
  WorkerIndex wi_;
};

// Mock lib::ThreadPool
using DefaultRunnable = ThreadPool;

// Give an assurance that executing time of the given function must
// less than specific time.
//
// Use macro instead of function because error message in function can
// only contain the most inner function location and outsider location
// would been ignored. It's not acceptable when many TIME_LESS
// functions exist outside.
#define TIME_LESS(time, func)                \
  ({                                         \
    const auto start_ts = co_current_time(); \
    {                                        \
      func();                                \
    }                                        \
    const auto end_ts = co_current_time();   \
    EXPECT_LT(end_ts - start_ts, (time));    \
  })

#define TIME_MORE(time, func)                \
  ({                                         \
    const auto start_ts = co_current_time(); \
    {                                        \
      func();                                \
    }                                        \
    const auto end_ts = co_current_time();   \
    EXPECT_GT(end_ts - start_ts, (time));    \
  })

}  // namespace cotesting

#endif /* CORO_TESTING_H */
