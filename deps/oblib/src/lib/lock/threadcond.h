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

#ifndef THREAD_COND_H_
#define THREAD_COND_H_

#include "lib/lock/ob_thread_cond.h"

namespace oceanbase {
namespace obsys {
class ThreadCond {
public:
  ThreadCond() {
    cond_.init();
  }
  ~ThreadCond() {
      cond_.destroy();
  }
  int lock() {
    return cond_.lock();
  }
  int unlock() {
    return cond_.unlock();
  }
  bool wait(int milliseconds = 0) {
    return oblib::OB_SUCCESS == cond_.wait(milliseconds);
  }
  void signal() {
      cond_.signal();
  }
  void broadcast() {
      cond_.broadcast();
  }

private:
  oceanbase::common::ObThreadCond cond_;
};

}  // namespace obsys
}  // namespace oceanbase

#endif /*THREAD_COND_H_*/
