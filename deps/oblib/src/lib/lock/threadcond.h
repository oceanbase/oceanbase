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

#ifndef TBSYS_COND_H_
#define TBSYS_COND_H_

#include "lib/lock/ob_thread_cond.h"

namespace oceanbase {
namespace obsys {
class CThreadCond {
public:
  CThreadCond()
  {
    cond_.init();
  }

  ~CThreadCond()
  {
    cond_.destroy();
  }

  int lock()
  {
    return cond_.lock();
  }

  int unlock()
  {
    return cond_.unlock();
  }

  bool wait(int milliseconds = 0)
  {
    return OB_SUCCESS == cond_.wait(milliseconds);
  }

  void signal()
  {
    cond_.signal();
  }

  void broadcast()
  {
    cond_.broadcast();
  }

private:
  oceanbase::common::ObThreadCond cond_;
};

class CCondGuard {
public:
  explicit CCondGuard(CThreadCond* cond)
  {
    cond_ = NULL;
    if (NULL != cond) {
      cond_ = cond;
      cond_->lock();
    }
  }

  ~CCondGuard()
  {
    if (NULL != cond_) {
      cond_->unlock();
    }
  }

private:
  CThreadCond* cond_;
};

}  // namespace obsys
}  // namespace oceanbase

#endif /*COND_H_*/
