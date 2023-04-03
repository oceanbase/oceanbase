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

#ifndef OCEANBASE_TRANSACTION_TEST_OB_ASYNC_UTILS_
#define OCEANBASE_TRANSACTION_TEST_OB_ASYNC_UTILS_
#include "lib/thread/ob_simple_thread_pool.h"
namespace oceanbase {
namespace test {
class Async {
public:
  class AsyncRunner : public common::ObSimpleThreadPool {
  public:
    AsyncRunner() {
      ObSimpleThreadPool::init(2, 256, "AsyncRunner-for-Testing");
    }
    void handle(void *task) {
      Async *async = (Async*)task;
      async->eval();
    }
  };

  Async(std::function<int(void)> &f) {
    static AsyncRunner runner;
    started_ = false;
    evaluated_ = false;
    f_ = f;
    runner.push(this);
  }
  void wait_started() {
    while(!ATOMIC_LOAD(&started_)) {
      usleep(1000);
    }
  }
  void eval() {
    ATOMIC_STORE(&started_, true);
    ret_ = f_();
    evaluated_ = true;
  }
  bool is_evaled() const { return evaluated_; }
  int get() const {
    while(!evaluated_) usleep(100);
    return ret_;
  }
private:
  bool started_ = false;
  bool evaluated_ = false;
  std::function<int(void)> f_;
  int ret_;
};
template<typename F>
inline Async static make_async(F &f) {
  std::function<int(void)> ff = f;
  return Async(ff);
}
} // test
} // oceanbase
#endif
